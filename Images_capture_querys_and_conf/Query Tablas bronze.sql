-- DESEMPLEO IMF
CREATE OR REPLACE EXTERNAL TABLE `proyecto-integrador-dae-2025.bronze.bronze_desempleo_imf_ext`
OPTIONS (
  format = 'PARQUET',
  uris   = ['gs://dae-integrador-2025/lakehouse/bronze/macroeconomics/bronze_desempleo_imf_data/*']
);

-- INFLACIÓN 12M
CREATE OR REPLACE EXTERNAL TABLE `proyecto-integrador-dae-2025.bronze.bronze_inflacion_12m_ext`
OPTIONS (
  format = 'PARQUET',
  uris   = ['gs://dae-integrador-2025/lakehouse/bronze/macroeconomics/bronze_inflacion_12m_data/*']
);

-- TIPO DE CAMBIO
CREATE OR REPLACE EXTERNAL TABLE `proyecto-integrador-dae-2025.bronze.bronze_tipo_cambio_ext`
OPTIONS (
  format = 'PARQUET',
  uris   = ['gs://dae-integrador-2025/lakehouse/bronze/macroeconomics/bronze_tipo_cambio_data/*']
);

-- External table limpia (Parquet) para 2025 completo
CREATE OR REPLACE EXTERNAL TABLE `proyecto-integrador-dae-2025.bronze.simbad_bronze_parquet_ext`
OPTIONS (
  format = 'PARQUET',
  uris   = ['gs://dae-integrador-2025/lakehouse/bronze/simbad/simbad_carteras_aayp_hipotecarios/*']
);

