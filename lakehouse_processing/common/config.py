# lakehouse_processing/common/config.py
BUCKET = "dae-integrador-2025"
REGION = "us-west2"

# Lakehouse paths
GCS = {
    "landing": f"gs://{BUCKET}/lakehouse/landing/",     # CSV source data
    "bronze":  f"gs://{BUCKET}/lakehouse/bronze/",      # Parquet raw ingestion
    "silver":  f"gs://{BUCKET}/lakehouse/silver/",      # Cleaned & normalized
    "gold":    f"gs://{BUCKET}/lakehouse/gold/",        # Aggregated metrics
}

# Data sources configuration
DATASETS = {
    "simbad": {
        "landing": f"{GCS['landing']}simbad/simbad_carteras_aayp_hipotecarios/",
        "bronze": f"{GCS['bronze']}simbad/simbad_carteras_aayp_hipotecarios/",
        "encoding": ["UTF-8", "ISO-8859-1"],  # fallback encoding
        "partition_cols": ["anio", "mes"]
    },
    "macroeconomics": {
        "landing": f"{GCS['landing']}macroeconomics/",
        "bronze": f"{GCS['bronze']}macroeconomics/",
        "datasets": ["desempleo_imf", "inflacion_12m", "tipo_cambio"],
        "encoding": ["UTF-8"],
        "partition_cols": ["dt_captura"]
    }
}

# DataProc cluster configuration
DATAPROC = {
    "cluster_name": "cluster-integrador-2025",
    "region": REGION,
    "spark_configs": {
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
}
