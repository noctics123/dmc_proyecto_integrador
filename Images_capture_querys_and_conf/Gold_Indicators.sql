-- Dataset GOLD (por si falta)
CREATE SCHEMA IF NOT EXISTS `proyecto-integrador-dae-2025.gold`
OPTIONS (location = 'US');

-- Tabla GOLD
CREATE OR REPLACE TABLE `proyecto-integrador-dae-2025.gold.simbad_gold`
PARTITION BY DATE_TRUNC(periodo_date, MONTH)
CLUSTER BY provincia
AS
WITH
-- A) Base SIMBAD (filtra Asalariado Privado)
simbad AS (
  SELECT
    anio, mes, provincia, tipoCliente, actividad,
    deudaCapital, deudaVencida, deudaVencidaDe31A90Dias, cantidadCredito,
    valorDesembolso, valorGarantia, valorProvisionCapitalYRendimiento, deuda,
    genero, persona, moneda, sector, entidad, residencia
  FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
  WHERE tipoCliente = 'Asalariado Privado'
),

-- B) Agregación (replica lógica del notebook)
agg AS (
  SELECT
    anio AS ANIO,
    mes  AS MES,
    provincia AS PROVINCIA,
    tipoCliente AS TIPOCLIENTE,
    actividad   AS ACTIVIDAD,

    SUM(deudaCapital)                    AS DEUDACAPITAL,
    SUM(deudaVencida)                    AS DEUDAVENCIDA,
    SUM(deudaVencidaDe31A90Dias)         AS DEUDAVENCIDADE31A90DIAS,
    SUM(cantidadCredito)                 AS CANTIDADCREDITO,

    AVG(CASE WHEN genero IN ('Género femenino','Genero femenino') THEN 1 ELSE 0 END)     AS GENERO,
    AVG(CASE WHEN persona IN ('Persona jurídica','Persona juridica') THEN 1 ELSE 0 END)  AS PERSONA,
    AVG(CASE WHEN moneda  IN ('Moneda Extranjera','MONEDA EXTRANJERA') THEN 1 ELSE 0 END) AS MONEDA,
    AVG(CASE WHEN sector  IN ('b) Privado','B) Privado','B) PRIVADO') THEN 1 ELSE 0 END) AS SECTOR,
    AVG(CASE WHEN entidad =  'APAP' THEN 1 ELSE 0 END)                                    AS ENTIDAD,
    AVG(CASE WHEN residencia IN ('a) Residente','A) Residente') THEN 1 ELSE 0 END)        AS RESIDENCIA,

    SUM(valorGarantia)                     AS VALORGARANTIA,
    SUM(valorProvisionCapitalYRendimiento) AS VALORPROVISIONCAPITALYRENDIMIENTO,
    SUM(valorDesembolso)                   AS VALORDESEMBOLSO,
    SUM(deuda)                             AS DEUDA
  FROM simbad
  GROUP BY ANIO, MES, PROVINCIA, TIPOCLIENTE, ACTIVIDAD
),

-- C) Métricas derivadas
agg2 AS (
  SELECT
    ANIO, MES, PROVINCIA, TIPOCLIENTE, ACTIVIDAD,
    DEUDACAPITAL, DEUDAVENCIDA, DEUDAVENCIDADE31A90DIAS, CANTIDADCREDITO,
    GENERO, PERSONA, MONEDA, SECTOR, ENTIDAD, RESIDENCIA,
    VALORGARANTIA, VALORPROVISIONCAPITALYRENDIMIENTO, VALORDESEMBOLSO, DEUDA,

    SAFE_DIVIDE(DEUDAVENCIDADE31A90DIAS, DEUDACAPITAL) AS TASA_MORA,
    SAFE_DIVIDE(VALORGARANTIA, DEUDACAPITAL)           AS COBERTURA_GARANTIA,
    SAFE_DIVIDE(VALORPROVISIONCAPITALYRENDIMIENTO, DEUDA) AS PROPORCION_PROVISIONADA,
    CASE WHEN DEUDAVENCIDA > 0
          AND SAFE_DIVIDE(DEUDAVENCIDADE31A90DIAS, DEUDACAPITAL) >= 0.005
         THEN 1 ELSE 0 END AS PD_AGREGADA
  FROM agg
),

-- D) Inflación mensual (usa la externa bronze_inflacion_12m_ext)
inflacion AS (
  SELECT
    EXTRACT(YEAR  FROM SAFE.PARSE_DATE('%Y-%m', FECHA))  AS ANIO,
    EXTRACT(MONTH FROM SAFE.PARSE_DATE('%Y-%m', FECHA))  AS MES,
    SAFE_CAST(REPLACE(INFLACION, ',', '.') AS FLOAT64)   AS INFLACION
  FROM `proyecto-integrador-dae-2025.bronze.bronze_inflacion_12m_ext`
  -- si tu archivo trae varios países, descomenta y ajusta:
  -- WHERE UPPER(PAIS) = 'DOMINICAN REPUBLIC'
),

-- E) Tipo de cambio mensual (promedio por mes) desde bronze_tipo_cambio_ext
tipo_cambio AS (
  SELECT
    EXTRACT(YEAR  FROM SAFE.PARSE_DATE('%Y-%m-%d', FECHA)) AS ANIO,
    EXTRACT(MONTH FROM SAFE.PARSE_DATE('%Y-%m-%d', FECHA)) AS MES,
    AVG(SAFE_CAST(REPLACE(TC_VENTA,  ',', '.') AS FLOAT64)) AS TC_VENTA,
    AVG(SAFE_CAST(REPLACE(TC_COMPRA, ',', '.') AS FLOAT64)) AS TC_COMPRA
  FROM `proyecto-integrador-dae-2025.bronze.bronze_tipo_cambio_ext`
  GROUP BY ANIO, MES
)

-- F) Resultado final (equivalente a df_final)
SELECT
  DATE(ANIO, MES, 1)  AS periodo_date,
  ANIO,
  MES,
  PROVINCIA,
  DEUDACAPITAL,
  DEUDAVENCIDA,
  DEUDAVENCIDADE31A90DIAS,
  CANTIDADCREDITO,
  GENERO,
  PERSONA,
  MONEDA,
  SECTOR,
  ENTIDAD,
  RESIDENCIA,
  VALORGARANTIA,
  VALORPROVISIONCAPITALYRENDIMIENTO,
  VALORDESEMBOLSO,
  DEUDA,
  TASA_MORA,
  COBERTURA_GARANTIA,
  PROPORCION_PROVISIONADA,
  PD_AGREGADA,
  i.INFLACION,
  t.TC_VENTA,
  t.TC_COMPRA
FROM agg2 a
LEFT JOIN inflacion   i USING (ANIO, MES)
LEFT JOIN tipo_cambio t USING (ANIO, MES);
