# etl_spark/common/config.py
BUCKET = "dmc_proy_storage"
REGION = "us-west2"

GCS = {
    "landing": f"gs://{BUCKET}/landing/dt=*/",     # CSV
    "bronze":  f"gs://{BUCKET}/bronze/",
    "silver":  f"gs://{BUCKET}/silver/",
    "gold":    f"gs://{BUCKET}/gold/",
}
