#!/usr/bin/env python
# coding: utf-8

# In[3]:


# ==============================================
# SIMBAD landing -> bronze (schema estable)
# ==============================================
from pyspark.sql import functions as F
from datetime import datetime

BUCKET = "dae-integrador-2025"
LANDING = f"gs://{BUCKET}/lakehouse/landing/simbad/simbad_carteras_aayp_hipotecarios"
BRONZE_BASE = f"gs://{BUCKET}/lakehouse/bronze/simbad/simbad_carteras_aayp_hipotecarios"

# --- utilidades para dt ---
def _list_subdirs(gcs_dir: str):
    jsc = sc._jsc
    hconf = jsc.hadoopConfiguration()
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(Path(gcs_dir).toUri(), hconf)
    return [st.getPath().getName() for st in fs.listStatus(Path(gcs_dir)) if st.isDirectory()]

def _pick_latest_dt_dir(base_dir: str):
    subdirs = _list_subdirs(base_dir)
    dt_dirs = [d for d in subdirs if d.startswith("dt=")]
    latest = max(dt_dirs, key=lambda d: datetime.strptime(d.split("=")[1], "%Y-%m-%d"))
    return latest, latest.split("=")[1]

# --- lee CSV del mayor dt (con fallback de encoding) ---
def _read_csv(path_glob: str):
    try:
        df = (spark.read
              .option("header", "true")
              .option("mode", "PERMISSIVE")
              .option("multiLine", "false")
              .option("inferSchema", "false")     # todo como STRING
              .option("encoding", "UTF-8")
              .csv(path_glob))
    except Exception:
        df = (spark.read
              .option("header", "true")
              .option("mode", "PERMISSIVE")
              .option("multiLine", "false")
              .option("inferSchema", "false")
              .option("encoding", "ISO-8859-1")
              .csv(path_glob))
    return df.select("*")

# --- columnas esperadas (en camelCase) ---
EXPECTED = [
    "periodo","tipoCredito","tipoEntidad","entidad","sectorEconomico","region","provincia",
    "moneda","tipoCartera","actividad","sector","persona","facilidad","residencia",
    "administracionYPropiedad","genero","tipoCliente","clasificacionEntidad",
    "cantidadPlasticos","cantidadCredito","deuda","tasaPorDeuda","deudaCapital",
    "deudaVencida","deudaVencidaDe31A90Dias","valorDesembolso","valorGarantia",
    "valorProvisionCapitalYRendimiento","__periodo"
]

# --- ingest ---
dt_dir, dt_str = _pick_latest_dt_dir(LANDING)
csv_glob = f"{LANDING}/{dt_dir}/*.csv"
print(f"[SIMBAD] leyendo: {csv_glob}")

df_raw = _read_csv(csv_glob)

# Asegura presencia/orden de columnas
sel = [ (F.col(c) if c in df_raw.columns else F.lit(None).alias(c)) for c in EXPECTED ]
df = df_raw.select(*sel)

# Normaliza y TIPOS FIJOS (un solo select)
df_cast = df.select(
    F.trim(F.col("periodo")).alias("periodo"),
    F.trim(F.col("tipoCredito")).alias("tipoCredito"),
    F.trim(F.col("tipoEntidad")).alias("tipoEntidad"),
    F.trim(F.col("entidad")).alias("entidad"),
    F.trim(F.col("sectorEconomico")).alias("sectorEconomico"),
    F.trim(F.col("region")).alias("region"),
    F.trim(F.col("provincia")).alias("provincia"),
    F.trim(F.col("moneda")).alias("moneda"),
    F.trim(F.col("tipoCartera")).alias("tipoCartera"),
    F.trim(F.col("actividad")).alias("actividad"),
    F.trim(F.col("sector")).alias("sector"),
    F.trim(F.col("persona")).alias("persona"),
    F.trim(F.col("facilidad")).alias("facilidad"),
    F.trim(F.col("residencia")).alias("residencia"),
    F.trim(F.col("administracionYPropiedad")).alias("administracionYPropiedad"),
    F.trim(F.col("genero")).alias("genero"),
    F.trim(F.col("tipoCliente")).alias("tipoCliente"),
    F.trim(F.col("clasificacionEntidad")).alias("clasificacionEntidad"),

    F.col("cantidadPlasticos").cast("int").alias("cantidadPlasticos"),
    F.col("cantidadCredito").cast("int").alias("cantidadCredito"),
    F.col("deuda").cast("double").alias("deuda"),
    F.col("tasaPorDeuda").cast("double").alias("tasaPorDeuda"),
    F.col("deudaCapital").cast("double").alias("deudaCapital"),
    F.col("deudaVencida").cast("double").alias("deudaVencida"),
    F.col("deudaVencidaDe31A90Dias").cast("double").alias("deudaVencidaDe31A90Dias"),
    F.col("valorDesembolso").cast("double").alias("valorDesembolso"),
    F.col("valorGarantia").cast("double").alias("valorGarantia"),
    F.col("valorProvisionCapitalYRendimiento").cast("double").alias("valorProvisionCapitalYRendimiento"),

    F.col("__periodo").alias("__periodo"),

    # derivados y trazabilidad
    F.to_date(F.col("periodo"), "yyyy-MM").alias("periodo_date"),
    F.year(F.to_date(F.col("periodo"), "yyyy-MM")).alias("anio"),
    F.month(F.to_date(F.col("periodo"), "yyyy-MM")).alias("mes"),
    F.lit(dt_str).alias("dt_captura"),
    F.input_file_name().alias("archivo_origen")
)

# --- escritura por año (carpeta) y partición por mes ---
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
anios = [r.anio for r in df_cast.select("anio").distinct().collect()]
for y in anios:
    out = f"{BRONZE_BASE}/anio={y}"
    (df_cast.filter(F.col("anio")==y)
            .write.mode("overwrite")
            .partitionBy("mes")
            .parquet(out))
    print(f"[SIMBAD] escrito -> {out}")


# In[ ]:




