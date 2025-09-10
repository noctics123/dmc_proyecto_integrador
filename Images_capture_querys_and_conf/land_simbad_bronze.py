#!/usr/bin/env python
# coding: utf-8

# In[2]:


# landing_to_bronze.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import input_file_name, current_date, year, month, col

spark = (
    SparkSession.builder
    .appName("landing_to_bronze")
    # Si usas DELTA, deja estas 2 líneas y cambia .format("parquet") por "delta"
    # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

bucket         = "dae-integrador-2025"
system_source  = "simbad"
table          = "simbad_carteras_aayp_hipotecarios"
landing_dt     = "2025-08-12"

path_landing = f"gs://{bucket}/lakehouse/landing/{system_source}/{table}/dt={landing_dt}/*.csv"
path_bronze  = f"gs://{bucket}/lakehouse/bronze/{system_source}/{table}/"

def delete_path_if_exists(spark_sess: SparkSession, path: str):
    hconf = spark_sess._jsc.hadoopConfiguration()
    jvm = spark_sess._jvm
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)
    p = jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(p):
        fs.delete(p, True)

# Limpia Bronze completo 
delete_path_if_exists(spark, path_bronze)

schema = (
    StructType()
    .add("PERIODO", StringType())
    .add("PROVINCIA", StringType())
    .add("TIPOCLIENTE", StringType())
    .add("ACTIVIDAD", StringType())
    .add("DEUDACAPITAL", StringType())
    .add("DEUDAVENCIDA", StringType())
    .add("DEUDAVENCIDADE31A90DIAS", StringType())
    .add("CANTIDADCREDITO", StringType())
    .add("GENERO", StringType())
    .add("PERSONA", StringType())
    .add("MONEDA", StringType())
    .add("SECTOR", StringType())
    .add("ENTIDAD", StringType())
    .add("RESIDENCIA", StringType())
    .add("VALORGARANTIA", StringType())
    .add("VALORPROVISIONCAPITALYRENDIMIENTO", StringType())
    .add("VALORDESEMBOLSO", StringType())
    .add("DEUDA", StringType())
)

df_raw = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    # .option("encoding", "UTF-8")        # si lo necesitas
    .schema(schema)
    .csv(path_landing)
)

if df_raw.rdd.isEmpty():
    print("Landing vacío; no se escribe Bronze.")
else:
    df_bronze = (
        df_raw
        .withColumn("fecha_proceso", current_date())
        .withColumn("anio", year(col("fecha_proceso")))
        .withColumn("mes", month(col("fecha_proceso")))
        .withColumn("archivo_origen", input_file_name())
    )

    (
        df_bronze.write
        .mode("overwrite")
        .format("parquet")        # usa "delta" si habilitas Delta arriba
        .partitionBy("anio", "mes")
        .save(path_bronze)
    )
    print("Bronze escrito en:", path_bronze)

spark.stop()



# In[1]:


# =========================================================
# 0) Parámetros
# =========================================================
BUCKET = "dae-integrador-2025"

LANDING_BASE = f"gs://{BUCKET}/lakehouse/landing/simbad/simbad_carteras_aayp_hipotecarios"
BRONZE_BASE  = f"gs://{BUCKET}/lakehouse/bronze/simbad/simbad_carteras_aayp_hipotecarios/anio=2025"

# Opcional: patrón de nombre del archivo (siempre cae el consolidado*)
CSV_GLOB = "consolidado_*.csv"   # usa '*.csv' si varía
ENCODING = "ISO-8859-1"           # corrige caracteres (Ã©, Ã±, etc.)

# =========================================================
# 1) Descubrir el último dt=YYYY-MM-DD en landing
# =========================================================
from pyspark.sql import functions as F, types as T
from datetime import datetime

# Utilidad: listar subdirectorios con Hadoop FS
def list_gcs_subdirs(gcs_dir: str):
    """
    Lista subdirectorios inmediatos en gcs_dir usando Hadoop FileSystem.
    Retorna lista de nombres (no paths completos).
    """
    jsc = sc._jsc
    hconf = jsc.hadoopConfiguration()
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    uri = Path(gcs_dir).toUri()
    fs = FileSystem.get(uri, hconf)

    statuses = fs.listStatus(Path(gcs_dir))
    subdirs = []
    for st in statuses:
        if st.isDirectory():
            name = st.getPath().getName()
            subdirs.append(name)
    return subdirs

# Listar dt=*
all_dirs = list_gcs_subdirs(LANDING_BASE)

# Filtrar los que cumplen patrón dt=YYYY-MM-DD y tomar el mayor por fecha
def is_dt_dir(d):
    return d.startswith("dt=") and len(d) == len("dt=YYYY-MM-DD")

def parse_dt(d):
    return datetime.strptime(d.split("=",1)[1], "%Y-%m-%d")

dt_dirs = [d for d in all_dirs if is_dt_dir(d)]
if not dt_dirs:
    raise RuntimeError(f"No se encontró ninguna carpeta dt=* en {LANDING_BASE}")

latest_dt_dir = max(dt_dirs, key=parse_dt)
latest_dt_str = latest_dt_dir.split("=")[1]

print(f"Último dt encontrado: {latest_dt_dir}")

# =========================================================
# 2) Construir ruta CSV a leer
# =========================================================
csv_path = f"{LANDING_BASE}/{latest_dt_dir}/{CSV_GLOB}"
print(f"Leyendo: {csv_path}")

# =========================================================
# 3) Definir esquema (tipos numéricos para métricas)
# =========================================================
schema = T.StructType([
    T.StructField("periodo", T.StringType(), True),
    T.StructField("tipoCredito", T.StringType(), True),
    T.StructField("tipoEntidad", T.StringType(), True),
    T.StructField("entidad", T.StringType(), True),
    T.StructField("sectorEconomico", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("provincia", T.StringType(), True),
    T.StructField("moneda", T.StringType(), True),
    T.StructField("tipoCartera", T.StringType(), True),
    T.StructField("actividad", T.StringType(), True),
    T.StructField("sector", T.StringType(), True),
    T.StructField("persona", T.StringType(), True),
    T.StructField("facilidad", T.StringType(), True),
    T.StructField("residencia", T.StringType(), True),
    T.StructField("administracionYPropiedad", T.StringType(), True),
    T.StructField("genero", T.StringType(), True),
    T.StructField("tipoCliente", T.StringType(), True),
    T.StructField("clasificacionEntidad", T.StringType(), True),
    T.StructField("cantidadPlasticos", T.IntegerType(), True),
    T.StructField("cantidadCredito", T.IntegerType(), True),
    T.StructField("deuda", T.DoubleType(), True),
    T.StructField("tasaPorDeuda", T.DoubleType(), True),
    T.StructField("deudaCapital", T.DoubleType(), True),
    T.StructField("deudaVencida", T.DoubleType(), True),
    T.StructField("deudaVencidaDe31A90Dias", T.DoubleType(), True),
    T.StructField("valorDesembolso", T.DoubleType(), True),
    T.StructField("valorGarantia", T.DoubleType(), True),
    T.StructField("valorProvisionCapitalYRendimiento", T.DoubleType(), True),
    T.StructField("__periodo", T.StringType(), True),
])

# =========================================================
# 4) Lectura del CSV (sin withColumn / sin auto-reasignación)
# =========================================================
df_raw_sel = (
    spark.read
        .option("header", "true")
        .option("multiLine", "false")
        .option("mode", "DROPMALFORMED")
        .option("encoding", ENCODING)
        .schema(schema)
        .csv(csv_path)
        .select("*")  # placeholder para mantener patrón _sel
)
df_raw = df_raw_sel

# =========================================================
# 5) Normalización mínima y columnas auxiliares (mes)
#    - Elegimos columna de mes desde 'periodo' (YYYY-MM)
# =========================================================
# Evitar withColumn: construir lista de expresiones para select único
select_exprs = [
    F.col("periodo"),
    F.col("tipoCredito"),
    F.col("tipoEntidad"),
    F.col("entidad"),
    F.col("sectorEconomico"),
    F.col("region"),
    F.col("provincia"),
    F.col("moneda"),
    F.col("tipoCartera"),
    F.col("actividad"),
    F.col("sector"),
    F.col("persona"),
    F.col("facilidad"),
    F.col("residencia"),
    F.col("administracionYPropiedad"),
    F.col("genero"),
    F.col("tipoCliente"),
    F.col("clasificacionEntidad"),
    F.col("cantidadPlasticos"),
    F.col("cantidadCredito"),
    F.col("deuda"),
    F.col("tasaPorDeuda"),
    F.col("deudaCapital"),
    F.col("deudaVencida"),
    F.col("deudaVencidaDe31A90Dias"),
    F.col("valorDesembolso"),
    F.col("valorGarantia"),
    F.col("valorProvisionCapitalYRendimiento"),
    F.col("__periodo"),
    # Derivados
    F.substring(F.col("periodo"), 6, 2).cast("int").alias("mes"),
    F.lit(latest_dt_str).alias("dt_captura"),
]

df_norm_sel = df_raw.select(*select_exprs)
df_norm = df_norm_sel

# =========================================================
# 6) Validaciones rápidas
# =========================================================
print("Filas a escribir:", df_norm.count())
display(df_norm.limit(5))

# =========================================================
# 7) Escritura a BRONZE particionando por mes (APPEND)
#    Ruta fija anio=2025 (según requerimiento)
# =========================================================
(
    df_norm
      .write
      .mode("append")                # usar "append" para no pisar particiones existentes
      .partitionBy("mes")            # particionar por mes
      .parquet(BRONZE_BASE)          # salida en Parquet
)

print(f"Datos escritos en: {BRONZE_BASE} particionado por mes")


# In[ ]:




