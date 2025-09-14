-- =============================================
-- Bronze External Tables - Apuntan a Parquet en GCS
-- =============================================

-- SIMBAD External Table
CREATE OR REPLACE EXTERNAL TABLE `proyecto-integrador-dae-2025.bronze.simbad_bronze_parquet_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dae-integrador-2025/lakehouse/bronze/simbad/simbad_carteras_aayp_hipotecarios/anio=*/mes=*/*.parquet']
);

-- Macroeconomics External Tables
CREATE OR REPLACE EXTERNAL TABLE `proyecto-integrador-dae-2025.bronze.bronze_desempleo_imf_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dae-integrador-2025/lakehouse/bronze/macroeconomics/bronze_desempleo_imf_data/dt_captura=*/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `proyecto-integrador-dae-2025.bronze.bronze_inflacion_12m_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dae-integrador-2025/lakehouse/bronze/macroeconomics/bronze_inflacion_12m_data/dt_captura=*/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `proyecto-integrador-dae-2025.bronze.bronze_tipo_cambio_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dae-integrador-2025/lakehouse/bronze/macroeconomics/bronze_tipo_cambio_data/dt_captura=*/*.parquet']
);

-- =============================================
-- Notas de Configuración
-- =============================================

-- Las external tables se actualizan automáticamente cuando:
-- 1. DataProc notebooks escriben nuevos archivos Parquet
-- 2. Los paths con wildcards (*) detectan nuevas particiones

-- Particionado en GCS:
-- - SIMBAD: /anio=YYYY/mes=MM/
-- - Macroeconomics: /dt_captura=YYYY-MM-DD/

-- Para debugging:
-- SELECT _FILE_NAME, COUNT(*) FROM bronze.simbad_bronze_parquet_ext GROUP BY 1 ORDER BY 1;