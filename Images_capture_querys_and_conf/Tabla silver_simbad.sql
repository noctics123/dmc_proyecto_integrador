-- MERGE incremental a Silver con dedupe + campos faltantes
MERGE `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios` T
USING (
  WITH tgt AS (
    SELECT IFNULL(MAX(periodo_ym), 0) AS max_ym
    FROM `proyecto-integrador-dae-2025.silver_clean.simbad_hipotecarios`
  ),
  -- Origen normalizado desde BRONZE (fix_ext si es el que usas)
  src_raw AS (
    SELECT
      REGEXP_REPLACE(TRIM(periodo),       r'\s+', ' ') AS periodo,
      REGEXP_REPLACE(TRIM(tipoCliente),   r'\s+', ' ') AS tipoCliente,
      REGEXP_REPLACE(TRIM(actividad),     r'\s+', ' ') AS actividad,
      REGEXP_REPLACE(TRIM(entidad),       r'\s+', ' ') AS entidad,
      REGEXP_REPLACE(TRIM(sector),        r'\s+', ' ') AS sector,
      REGEXP_REPLACE(TRIM(moneda),        r'\s+', ' ') AS moneda,
      REGEXP_REPLACE(TRIM(provincia),     r'\s+', ' ') AS provincia,
      REGEXP_REPLACE(TRIM(residencia),    r'\s+', ' ') AS residencia,
      REGEXP_REPLACE(TRIM(genero),        r'\s+', ' ') AS genero,
      REGEXP_REPLACE(TRIM(persona),       r'\s+', ' ') AS persona,

      SAFE_CAST(deudaCapital AS FLOAT64)                 AS deudaCapital,
      SAFE_CAST(deudaVencida AS FLOAT64)                 AS deudaVencida,
      SAFE_CAST(deudaVencidaDe31A90Dias AS FLOAT64)      AS deudaVencidaDe31A90Dias,
      SAFE_CAST(cantidadCredito AS INT64)                AS cantidadCredito,
      SAFE_CAST(valorDesembolso AS FLOAT64)              AS valorDesembolso,
      SAFE_CAST(valorGarantia AS FLOAT64)                AS valorGarantia,
      SAFE_CAST(valorProvisionCapitalYRendimiento AS FLOAT64) AS valorProvisionCapitalYRendimiento,
      SAFE_CAST(deuda AS FLOAT64)                        AS deuda,

      COALESCE(periodo_date, SAFE.PARSE_DATE('%Y-%m', TRIM(periodo))) AS periodo_date,
      EXTRACT(YEAR  FROM COALESCE(periodo_date, SAFE.PARSE_DATE('%Y-%m', TRIM(periodo))))  AS anio,
      EXTRACT(MONTH FROM COALESCE(periodo_date, SAFE.PARSE_DATE('%Y-%m', TRIM(periodo))))  AS mes,
      CAST(FORMAT_DATE('%Y%m', COALESCE(periodo_date, SAFE.PARSE_DATE('%Y-%m', TRIM(periodo)))) AS INT64) AS periodo_ym,

      SAFE.PARSE_DATE('%Y-%m-%d', dt_captura) AS dt_captura,
      archivo_origen
    FROM `proyecto-integrador-dae-2025.bronze.simbad_bronze_parquet_ext`
  ),
  -- DEDUPE: 1 fila por clave natural (la más reciente por dt_captura, y en empate por archivo)
  src AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT src_raw.*,
             ROW_NUMBER() OVER (
               PARTITION BY periodo, entidad, provincia, moneda, tipoCliente, actividad, sector, residencia
               ORDER BY dt_captura DESC, archivo_origen DESC
             ) AS rn
      FROM src_raw
    )
    WHERE rn = 1
  )
  SELECT s.*
  FROM src s
  CROSS JOIN tgt
  WHERE s.periodo_ym >= tgt.max_ym     -- incremental: último período + nuevos
) S
ON (
  T.periodo     = S.periodo
  AND T.entidad = S.entidad
  AND T.provincia = S.provincia
  AND T.moneda  = S.moneda
  AND T.tipoCliente = S.tipoCliente
  AND T.actividad   = S.actividad
  AND T.sector      = S.sector
  AND T.residencia  = S.residencia
)
WHEN MATCHED THEN UPDATE SET
  deudaCapital = S.deudaCapital,
  deudaVencida = S.deudaVencida,
  deudaVencidaDe31A90Dias = S.deudaVencidaDe31A90Dias,
  cantidadCredito = S.cantidadCredito,
  valorDesembolso = S.valorDesembolso,
  valorGarantia   = S.valorGarantia,
  valorProvisionCapitalYRendimiento = S.valorProvisionCapitalYRendimiento,
  deuda         = S.deuda,
  periodo_date  = S.periodo_date,
  anio          = S.anio,
  mes           = S.mes,
  periodo_ym    = S.periodo_ym,
  dt_captura    = S.dt_captura,
  genero        = S.genero,
  persona       = S.persona
WHEN NOT MATCHED THEN
  INSERT (
    periodo, tipoCliente, actividad, entidad, sector, moneda, provincia, residencia,
    deudaCapital, deudaVencida, deudaVencidaDe31A90Dias, cantidadCredito,
    valorDesembolso, valorGarantia, valorProvisionCapitalYRendimiento, deuda,
    periodo_date, anio, mes, periodo_ym, dt_captura, genero, persona
  )
  VALUES (
    S.periodo, S.tipoCliente, S.actividad, S.entidad, S.sector, S.moneda, S.provincia, S.residencia,
    S.deudaCapital, S.deudaVencida, S.deudaVencidaDe31A90Dias, S.cantidadCredito,
    S.valorDesembolso, S.valorGarantia, S.valorProvisionCapitalYRendimiento, S.deuda,
    S.periodo_date, S.anio, S.mes, S.periodo_ym, S.dt_captura, S.genero, S.persona
  );
