import os, tempfile, datetime as dt
from typing import Optional, Dict, Any, List
import pandas as pd, requests
from fastapi import FastAPI, Body
from google.cloud import storage

BUCKET = os.getenv("GCS_BUCKET", "dmc_proy_storage")
DATASET_PREFIX = os.getenv("LANDING_PREFIX", "landing")

app = FastAPI(title="Macroeconomics Scraper", version="1.0.0")

def save_pandas_to_gcs(df: pd.DataFrame, bucket: str, object_name: str) -> str:
    client = storage.Client()
    bkt = client.bucket(bucket)
    blob = bkt.blob(object_name)
    with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
        df.to_csv(tmp.name, index=False, encoding="utf-8")
        blob.upload_from_filename(tmp.name, content_type="text/csv")
    return f"gs://{bucket}/{object_name}"

def date_part(run_date: Optional[str]) -> str:
    return f"dt={(run_date or dt.date.today().isoformat())}"

# === Reemplaza estos ejemplos con tu extracción real ===
def extract_inflacion_12m() -> pd.DataFrame:
    return pd.DataFrame([{"pais":"Peru","fecha":"2025-08","inflacion12m":3.1}])

def extract_tipo_cambio() -> pd.DataFrame:
    return pd.DataFrame([{"fecha":"2025-08-01","tc_compra":3.72,"tc_venta":3.74}])

def extract_desempleo_imf() -> pd.DataFrame:
    return pd.DataFrame([{"pais":"Peru","anio":2024,"tasa_desempleo":7.4}])
# =======================================================

def run_pipeline(run_date: Optional[str]=None) -> Dict[str, Any]:
    saved: List[str] = []
    part = date_part(run_date)

    df1 = extract_inflacion_12m()
    if not df1.empty:
        saved.append(save_pandas_to_gcs(df1, BUCKET, f"{DATASET_PREFIX}/{part}/inflacion_12m.csv"))

    df2 = extract_tipo_cambio()
    if not df2.empty:
        saved.append(save_pandas_to_gcs(df2, BUCKET, f"{DATASET_PREFIX}/{part}/tipo_cambio.csv"))

    df3 = extract_desempleo_imf()
    if not df3.empty:
        saved.append(save_pandas_to_gcs(df3, BUCKET, f"{DATASET_PREFIX}/{part}/desempleo_imf.csv"))

    return {"saved": saved, "date_partition": part}

@app.get("/healthz")
def healthz(): return {"status":"ok"}

@app.post("/run")
def run(body: Dict[str, Any] = Body(default={})):
    return {"ok": True, **run_pipeline(body.get("run_date"))}

if __name__ == "__main__":
    import uvicorn, os
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT","8080")))
